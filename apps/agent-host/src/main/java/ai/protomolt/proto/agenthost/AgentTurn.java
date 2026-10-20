package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.delegation.v1.DeliverableContract;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * A validated set of delegation commands acknowledging an exact event batch.
 *
 * <p>A command is one delegation verb's request message. The tool names the message, the
 * arguments are that message's canonical proto3 JSON, and both the schema the model is
 * given and the check applied to what it returns come from the request descriptor. The
 * coordinator validates the same rules server-side; applying them here means a violation
 * is answered by a repair turn quoting the proto's own rule text rather than by a failed
 * tool call halfway through a batch.
 *
 * <p>This record keeps the turn's trust decisions: the reply size bound, the exact
 * cursor acknowledgement, the per-role tool allowlist and the identity pinning the
 * model may not override. The provider-neutral rendering and checking it delegates:
 * {@link AgentCommands} is the command catalog, {@link AgentReplyReader} finds the
 * JSON value in a reply, {@link AgentCommandValidation} applies the request
 * descriptors' rules, {@link AgentTurnSchemas} renders the structured-output schema
 * and {@link AgentCommandContract} renders the prose contract.
 */
record AgentTurn(List<Long> handledEventCursors, List<Command> commands) {

    private static final int MAX_RESPONSE_CHARS = 256 * 1024;

    record Command(String tool, ObjectNode arguments) {
        Command {
            arguments = arguments.deepCopy();
        }
    }

    static AgentTurn parse(String text, AgentRole role, List<Long> expectedCursors,
                           String identity) {
        return parse(text, role, expectedCursors, identity, Map.of());
    }

    static AgentTurn parse(String text, AgentRole role, List<Long> expectedCursors,
                           String identity, Map<String, DeliverableContract> contracts) {
        if (text == null || text.length() > MAX_RESPONSE_CHARS) {
            throw new ModelReplyException("agent response exceeds the 256 KiB limit");
        }
        JsonNode parsed = AgentReplyReader.readResponse(text);
        if (!parsed.isObject()) {
            throw new ModelReplyException("agent response must be one JSON object");
        }
        ObjectNode root = (ObjectNode) parsed;
        Set<String> fields = new HashSet<>();
        root.fieldNames().forEachRemaining(fields::add);
        if (!Set.of("handledEventCursors", "commands").containsAll(fields)) {
            throw new ModelReplyException("agent response contains unknown fields");
        }
        List<Long> cursors = readCursors(root.get("handledEventCursors"));
        if (!cursors.equals(expectedCursors)) {
            throw new ModelReplyException("handledEventCursors must exactly match "
                    + expectedCursors);
        }
        JsonNode commandNodes = root.get("commands");
        if (commandNodes == null || !commandNodes.isArray()
                || commandNodes.isEmpty() || commandNodes.size() > AgentCommands.MAX_COMMANDS) {
            throw new ModelReplyException("commands must contain 1 to "
                    + AgentCommands.MAX_COMMANDS + " entries");
        }
        List<String> allowed = tools(role);
        List<Command> commands = new ArrayList<>();
        for (int i = 0; i < commandNodes.size(); i++) {
            JsonNode node = commandNodes.get(i);
            if (!node.isObject() || !node.path("tool").isTextual()
                    || !node.path("arguments").isObject() || node.size() != 2) {
                throw new ModelReplyException("command " + i
                        + " must contain a tool and object arguments");
            }
            String tool = node.path("tool").asText();
            if (!allowed.contains(tool)) {
                throw new ModelReplyException("tool '" + tool + "' is not allowed for "
                        + role.name().toLowerCase(Locale.ROOT));
            }
            ObjectNode arguments = ((ObjectNode) node.path("arguments")).deepCopy();
            if (AgentCommands.ACK.equals(tool)) {
                AgentCommandValidation.validateAck(arguments);
            } else {
                enforceIdentity(role, identity, tool, arguments);
                AgentCommandValidation.validateRequest(tool, arguments, i, contracts);
            }
            commands.add(new Command(tool, arguments));
        }
        return new AgentTurn(List.copyOf(cursors), List.copyOf(commands));
    }

    /** The commands a role may issue, host-local acknowledgement first. */
    static List<String> tools(AgentRole role) {
        return AgentCommands.tools(role);
    }

    /** The structured-output schema for a role; see {@link AgentTurnSchemas}. */
    static ObjectNode outputSchema(AgentRole role) {
        return outputSchema(role, Map.of());
    }

    /** The structured-output schema for a role with its deliverable contracts. */
    static ObjectNode outputSchema(AgentRole role, Map<String, DeliverableContract> contracts) {
        return AgentTurnSchemas.outputSchema(role, contracts);
    }

    /** The prose argument contract for a role; see {@link AgentCommandContract}. */
    static String commandContract(AgentRole role) {
        return AgentCommandContract.commandContract(role);
    }

    private static List<Long> readCursors(JsonNode node) {
        if (node == null || !node.isArray()) {
            throw new ModelReplyException("handledEventCursors must be an array");
        }
        List<Long> cursors = new ArrayList<>();
        long previous = -1;
        for (JsonNode entry : node) {
            if (!entry.canConvertToLong() || entry.asLong() <= previous) {
                throw new ModelReplyException(
                        "handledEventCursors must be strictly increasing integers");
            }
            previous = entry.asLong();
            cursors.add(previous);
        }
        return cursors;
    }

    private static void enforceIdentity(AgentRole role, String identity, String tool,
                                        ObjectNode arguments) {
        if (role == AgentRole.WORKER) {
            if (AgentCommands.MESSAGE.equals(tool)) {
                requireCompatible(arguments, "sender", identity);
                requireCompatible(arguments, "recipient", "coordinator");
                arguments.put("sender", identity);
                arguments.put("recipient", "coordinator");
            } else {
                requireCompatible(arguments, "workerId", identity);
                arguments.put("workerId", identity);
            }
        } else if (AgentCommands.MESSAGE.equals(tool)) {
            requireCompatible(arguments, "sender", "coordinator");
            arguments.put("sender", "coordinator");
        }
    }

    private static void requireCompatible(ObjectNode arguments, String field, String value) {
        JsonNode present = arguments.get(field);
        if (present != null && (!present.isTextual() || !value.equals(present.asText()))) {
            throw new ModelReplyException("agent cannot override " + field);
        }
    }
}
