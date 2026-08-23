package ai.pipestream.proto.agenthost;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** A validated set of delegation commands acknowledging an exact event batch. */
record AgentTurn(List<Long> handledEventCursors, List<Command> commands) {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_COMMANDS = 16;
    private static final int MAX_RESPONSE_CHARS = 256 * 1024;
    /** The review decision that completes a task, as the review verb names it. */
    private static final String ACCEPT = "REVIEW_DECISION_ACCEPT";
    /** The review decision that returns a candidate for another revision. */
    private static final String REVISE = "REVIEW_DECISION_REVISE";
    private static final Set<String> WORKER_TOOLS = Set.of(
            "host-ack", "delegation-accept", "delegation-message",
            "delegation-progress", "delegation-checkpoint", "delegation-candidate");
    private static final Set<String> COORDINATOR_TOOLS = Set.of(
            "host-ack", "delegation-offer", "delegation-message",
            "delegation-review", "delegation-cancel");

    record Command(String tool, ObjectNode arguments) {
        Command {
            arguments = arguments.deepCopy();
        }
    }

    static AgentTurn parse(String text, AgentRole role, List<Long> expectedCursors,
                           String identity) {
        if (text == null || text.length() > MAX_RESPONSE_CHARS) {
            throw new AgentHostException("agent response exceeds the 256 KiB limit");
        }
        JsonNode parsed;
        try {
            parsed = MAPPER.readTree(text);
        } catch (JsonProcessingException e) {
            throw new AgentHostException("agent response is not JSON");
        }
        if (!parsed.isObject()) {
            throw new AgentHostException("agent response must be one JSON object");
        }
        ObjectNode root = (ObjectNode) parsed;
        Set<String> fields = new HashSet<>();
        root.fieldNames().forEachRemaining(fields::add);
        if (!Set.of("handledEventCursors", "commands").containsAll(fields)) {
            throw new AgentHostException("agent response contains unknown fields");
        }
        List<Long> cursors = readCursors(root.get("handledEventCursors"));
        if (!cursors.equals(expectedCursors)) {
            throw new AgentHostException("handledEventCursors must exactly match "
                    + expectedCursors);
        }
        JsonNode commandNodes = root.get("commands");
        if (commandNodes == null || !commandNodes.isArray()
                || commandNodes.isEmpty() || commandNodes.size() > MAX_COMMANDS) {
            throw new AgentHostException("commands must contain 1 to "
                    + MAX_COMMANDS + " entries");
        }
        Set<String> allowed = role == AgentRole.WORKER ? WORKER_TOOLS : COORDINATOR_TOOLS;
        List<Command> commands = new ArrayList<>();
        for (int i = 0; i < commandNodes.size(); i++) {
            JsonNode node = commandNodes.get(i);
            if (!node.isObject() || !node.path("tool").isTextual()
                    || !node.path("arguments").isObject() || node.size() != 2) {
                throw new AgentHostException("command " + i
                        + " must contain a tool and object arguments");
            }
            String tool = node.path("tool").asText();
            if (!allowed.contains(tool)) {
                throw new AgentHostException("tool '" + tool + "' is not allowed for "
                        + role.name().toLowerCase(java.util.Locale.ROOT));
            }
            ObjectNode arguments = ((ObjectNode) node.path("arguments")).deepCopy();
            if ("host-ack".equals(tool)) {
                validateAck(arguments);
            }
            enforceIdentity(role, identity, tool, arguments);
            validateArguments(role, tool, arguments, i);
            commands.add(new Command(tool, arguments));
        }
        return new AgentTurn(List.copyOf(cursors), List.copyOf(commands));
    }

    static ObjectNode outputSchema(AgentRole role) {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("handledEventCursors").put("type", "array")
                .putObject("items").put("type", "integer");
        ObjectNode commands = properties.putObject("commands");
        commands.put("type", "array").put("minItems", 1).put("maxItems", MAX_COMMANDS);
        ArrayNode variants = commands.putObject("items").putArray("anyOf");
        (role == AgentRole.WORKER ? WORKER_TOOLS : COORDINATOR_TOOLS).stream()
                .sorted()
                .flatMap(tool -> commandSchemas(role, tool).stream())
                .forEach(variants::add);
        schema.putArray("required").add("handledEventCursors").add("commands");
        schema.put("additionalProperties", false);
        return schema;
    }

    private static List<ObjectNode> commandSchemas(AgentRole role, String tool) {
        if ("delegation-review".equals(tool)) {
            return List.of(reviewSchema(ACCEPT), reviewSchema(REVISE));
        }
        ObjectNode arguments = switch (tool) {
            case "host-ack" -> objectSchema()
                    .set("properties", properties(
                            field("reason", stringSchema(1, 1_024))));
            case "delegation-accept" -> objectSchema()
                    .set("properties", properties(
                            field("taskId", uuidSchema()),
                            field("attempt", integerSchema(1, 1_024))));
            case "delegation-message" -> messageArguments(role);
            case "delegation-progress" -> objectSchema()
                    .set("properties", properties(
                            field("taskId", uuidSchema()),
                            field("attempt", integerSchema(1, 1_024)),
                            field("message", stringSchema(1, 4_096))));
            case "delegation-checkpoint" -> objectSchema()
                    .set("properties", properties(
                            field("taskId", uuidSchema()),
                            field("attempt", integerSchema(1, 1_024)),
                            field("resumeToken", stringSchema(1, 512)),
                            field("note", stringSchema(0, 1_024))));
            case "delegation-candidate" -> objectSchema()
                    .set("properties", properties(
                            field("taskId", uuidSchema()),
                            field("candidate", candidateSchema())));
            case "delegation-offer" -> objectSchema()
                    .set("properties", properties(
                            field("workerId", identitySchema()),
                            field("taskId", uuidSchema()),
                            field("leaseSeconds", integerSchema(1, 86_400)),
                            field("spec", taskSpecSchema())));
            case "delegation-cancel" -> objectSchema()
                    .set("properties", properties(
                            field("taskId", uuidSchema()),
                            field("reason", stringSchema(1, 2_048))));
            default -> throw new IllegalArgumentException("unknown agent-host tool: " + tool);
        };
        finishObject(arguments);
        return List.of(commandSchema(tool, arguments));
    }

    private static ObjectNode messageArguments(AgentRole role) {
        ObjectNode properties = properties(
                field("taskId", uuidSchema()),
                field("kind", enumSchema(
                        "TASK_MESSAGE_KIND_QUESTION", "TASK_MESSAGE_KIND_ANSWER",
                        "TASK_MESSAGE_KIND_GUIDANCE", "TASK_MESSAGE_KIND_NOTE")),
                field("text", stringSchema(1, 8_192)));
        if (role == AgentRole.COORDINATOR) {
            properties.set("recipient", identitySchema());
        }
        ObjectNode schema = objectSchema().set("properties", properties);
        finishObject(schema);
        return schema;
    }

    private static ObjectNode reviewSchema(String decision) {
        ObjectNode arguments;
        if (ACCEPT.equals(decision)) {
            arguments = objectSchema().set("properties", properties(
                    field("taskId", uuidSchema()),
                    field("decision", enumSchema(ACCEPT)),
                    field("verdict", stringSchema(1, 2_048))));
        } else {
            arguments = objectSchema().set("properties", properties(
                    field("taskId", uuidSchema()),
                    field("decision", enumSchema(REVISE)),
                    field("feedback", stringSchema(1, 8_192)),
                    field("failedChecks", arraySchema(stringSchema(1, 128), 0, 64))));
        }
        finishObject(arguments);
        return commandSchema("delegation-review", arguments);
    }

    private static ObjectNode commandSchema(String tool, ObjectNode arguments) {
        ObjectNode schema = objectSchema();
        schema.set("properties", properties(
                field("tool", enumSchema(tool)),
                field("arguments", arguments)));
        finishObject(schema);
        return schema;
    }

    private static ObjectNode taskSpecSchema() {
        ObjectNode check = objectSchema().set("properties", properties(
                field("name", stringSchema(1, 128)),
                field("description", stringSchema(0, 2_048))));
        finishObject(check);
        ObjectNode schema = objectSchema().set("properties", properties(
                field("objective", stringSchema(1, 16_384)),
                field("requiredChecks", arraySchema(check, 1, 64))));
        finishObject(schema);
        return schema;
    }

    private static ObjectNode candidateSchema() {
        ObjectNode evidence = objectSchema().set("properties", properties(
                field("checkName", stringSchema(1, 128)),
                field("verdict", enumSchema("CHECK_VERDICT_PASSED")),
                field("ranAt", stringSchema(1, 64)),
                field("detail", stringSchema(0, 4_096))));
        finishObject(evidence);
        ObjectNode commit = objectSchema().set("properties", properties(
                field("repository", stringSchema(1, 512)),
                field("commit", stringSchema(40, 40)),
                field("subject", stringSchema(0, 256))));
        finishObject(commit);
        ObjectNode schema = objectSchema().set("properties", properties(
                field("attempt", integerSchema(1, 1_024)),
                field("revision", integerSchema(1, 1_024)),
                field("summary", stringSchema(1, 4_096)),
                field("evidence", arraySchema(evidence, 1, 64)),
                field("commits", arraySchema(commit, 1, 64))));
        finishObject(schema);
        return schema;
    }

    private static ObjectNode objectSchema() {
        return MAPPER.createObjectNode().put("type", "object")
                .put("additionalProperties", false);
    }

    private static ObjectNode properties(Field... fields) {
        ObjectNode properties = MAPPER.createObjectNode();
        for (Field field : fields) {
            properties.set(field.name(), field.schema());
        }
        return properties;
    }

    private static Field field(String name, ObjectNode schema) {
        return new Field(name, schema);
    }

    private static void finishObject(ObjectNode schema) {
        ArrayNode required = schema.putArray("required");
        schema.path("properties").fieldNames().forEachRemaining(required::add);
    }

    private static ObjectNode stringSchema(int minLength, int maxLength) {
        return MAPPER.createObjectNode().put("type", "string")
                .put("minLength", minLength).put("maxLength", maxLength);
    }

    private static ObjectNode uuidSchema() {
        return stringSchema(36, 36).put("pattern",
                "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
                        + "[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$");
    }

    private static ObjectNode identitySchema() {
        return stringSchema(1, 128);
    }

    private static ObjectNode integerSchema(int minimum, int maximum) {
        return MAPPER.createObjectNode().put("type", "integer")
                .put("minimum", minimum).put("maximum", maximum);
    }

    private static ObjectNode enumSchema(String... values) {
        ObjectNode schema = MAPPER.createObjectNode().put("type", "string");
        ArrayNode choices = schema.putArray("enum");
        for (String value : values) {
            choices.add(value);
        }
        return schema;
    }

    private static ObjectNode arraySchema(ObjectNode items, int minimum, int maximum) {
        return MAPPER.createObjectNode().put("type", "array")
                .put("minItems", minimum).put("maxItems", maximum).set("items", items);
    }

    private record Field(String name, ObjectNode schema) {
    }

    private static List<Long> readCursors(JsonNode node) {
        if (node == null || !node.isArray()) {
            throw new AgentHostException("handledEventCursors must be an array");
        }
        List<Long> cursors = new ArrayList<>();
        long previous = -1;
        for (JsonNode entry : node) {
            if (!entry.canConvertToLong() || entry.asLong() <= previous) {
                throw new AgentHostException(
                        "handledEventCursors must be strictly increasing integers");
            }
            previous = entry.asLong();
            cursors.add(previous);
        }
        return cursors;
    }

    private static void enforceIdentity(AgentRole role, String identity, String tool,
                                        ObjectNode arguments) {
        if ("host-ack".equals(tool)) {
            return;
        }
        if (role == AgentRole.WORKER) {
            if ("delegation-message".equals(tool)) {
                requireCompatible(arguments, "sender", identity);
                requireCompatible(arguments, "recipient", "coordinator");
                arguments.put("sender", identity);
                arguments.put("recipient", "coordinator");
            } else {
                requireCompatible(arguments, "workerId", identity);
                arguments.put("workerId", identity);
            }
        } else if ("delegation-message".equals(tool)) {
            requireCompatible(arguments, "sender", "coordinator");
            arguments.put("sender", "coordinator");
        }
    }

    private static void requireCompatible(ObjectNode arguments, String field, String value) {
        JsonNode present = arguments.get(field);
        if (present != null && (!present.isTextual() || !value.equals(present.asText()))) {
            throw new AgentHostException("agent cannot override " + field);
        }
    }

    private static void validateAck(ObjectNode arguments) {
        if (arguments.size() != 1 || !arguments.path("reason").isTextual()
                || arguments.path("reason").asText().isBlank()
                || arguments.path("reason").asText().length() > 1_024) {
            throw new AgentHostException(
                    "host-ack arguments must contain one bounded reason");
        }
    }

    /**
     * Enforces the same closed command schema on provider text that is offered to
     * structured-output capable providers. ACP providers may return ordinary JSON,
     * so treating the advertised schema as validation rather than only a prompt hint
     * keeps malformed commands out of durable pending state.
     */
    private static void validateArguments(AgentRole role, String tool,
                                          ObjectNode arguments, int commandIndex) {
        ObjectNode candidate = arguments.deepCopy();
        if (role == AgentRole.WORKER) {
            if ("delegation-message".equals(tool)) {
                candidate.remove("sender");
                candidate.remove("recipient");
            } else if (!"host-ack".equals(tool)) {
                candidate.remove("workerId");
            }
        } else if ("delegation-message".equals(tool)) {
            candidate.remove("sender");
        }

        AgentHostException first = null;
        for (ObjectNode commandSchema : commandSchemas(role, tool)) {
            JsonNode argumentSchema = commandSchema.path("properties").path("arguments");
            try {
                validateNode(candidate, argumentSchema,
                        "command " + commandIndex + " arguments");
                return;
            } catch (AgentHostException e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        throw first == null
                ? new AgentHostException("command " + commandIndex
                        + " has no argument schema")
                : first;
    }

    private static void validateNode(JsonNode value, JsonNode schema, String path) {
        String type = schema.path("type").asText();
        switch (type) {
            case "object" -> validateObject(value, schema, path);
            case "array" -> validateArray(value, schema, path);
            case "string" -> validateString(value, schema, path);
            case "integer" -> validateInteger(value, schema, path);
            default -> throw new AgentHostException(path
                    + " uses an unsupported internal schema type");
        }
    }

    private static void validateObject(JsonNode value, JsonNode schema, String path) {
        if (!value.isObject()) {
            throw new AgentHostException(path + " must be an object");
        }
        JsonNode properties = schema.path("properties");
        for (JsonNode required : schema.path("required")) {
            String name = required.asText();
            if (!value.has(name)) {
                throw new AgentHostException(path + "." + name + " is required");
            }
        }
        Iterator<Map.Entry<String, JsonNode>> fields = value.properties().iterator();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            JsonNode fieldSchema = properties.get(field.getKey());
            if (fieldSchema == null) {
                throw new AgentHostException(path + " contains unknown field '"
                        + field.getKey() + "'");
            }
            validateNode(field.getValue(), fieldSchema, path + "." + field.getKey());
        }
    }

    private static void validateArray(JsonNode value, JsonNode schema, String path) {
        if (!value.isArray()) {
            throw new AgentHostException(path + " must be an array");
        }
        int minimum = schema.path("minItems").asInt(0);
        int maximum = schema.path("maxItems").asInt(Integer.MAX_VALUE);
        if (value.size() < minimum || value.size() > maximum) {
            throw new AgentHostException(path + " must contain " + minimum
                    + " to " + maximum + " entries");
        }
        for (int i = 0; i < value.size(); i++) {
            validateNode(value.get(i), schema.path("items"), path + "[" + i + "]");
        }
    }

    private static void validateString(JsonNode value, JsonNode schema, String path) {
        if (!value.isTextual()) {
            throw new AgentHostException(path + " must be a string");
        }
        String text = value.asText();
        int minimum = schema.path("minLength").asInt(0);
        int maximum = schema.path("maxLength").asInt(Integer.MAX_VALUE);
        if (text.length() < minimum || text.length() > maximum) {
            throw new AgentHostException(path + " length must be between " + minimum
                    + " and " + maximum);
        }
        if (schema.has("pattern")
                && !Pattern.matches(schema.path("pattern").asText(), text)) {
            throw new AgentHostException(path + " does not match its required form");
        }
        if (schema.has("enum")) {
            boolean matched = false;
            for (JsonNode choice : schema.path("enum")) {
                matched |= choice.asText().equals(text);
            }
            if (!matched) {
                throw new AgentHostException(path + " is not an allowed value");
            }
        }
    }

    private static void validateInteger(JsonNode value, JsonNode schema, String path) {
        if (!value.isIntegralNumber() || !value.canConvertToLong()) {
            throw new AgentHostException(path + " must be an integer");
        }
        long number = value.asLong();
        long minimum = schema.path("minimum").asLong(Long.MIN_VALUE);
        long maximum = schema.path("maximum").asLong(Long.MAX_VALUE);
        if (number < minimum || number > maximum) {
            throw new AgentHostException(path + " must be between " + minimum
                    + " and " + maximum);
        }
    }
}
