package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.delegation.v1.AcceptTaskRequest;
import ai.protomolt.proto.delegation.v1.CancelTaskRequest;
import ai.protomolt.proto.delegation.v1.OfferTaskRequest;
import ai.protomolt.proto.delegation.v1.RecordCheckpointRequest;
import ai.protomolt.proto.delegation.v1.ReportProgressRequest;
import ai.protomolt.proto.delegation.v1.ReviewCandidateRequest;
import ai.protomolt.proto.delegation.v1.SendTaskMessageRequest;
import ai.protomolt.proto.delegation.v1.SubmitCandidateRequest;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import ai.protomolt.proto.validate.model.CelConstraint;
import ai.protomolt.proto.validate.model.MessageConstraints;
import ai.protomolt.proto.validate.spi.ValidationRuleSource;
import ai.protomolt.proto.validate.spi.ValidationRuleSources;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A validated set of delegation commands acknowledging an exact event batch.
 *
 * <p>A command is one delegation verb's request message. The tool names the message, the
 * arguments are that message's canonical proto3 JSON, and both the schema the model is
 * given and the check applied to what it returns come from the request descriptor. The
 * coordinator validates the same rules server-side; applying them here means a violation
 * is answered by a repair turn quoting the proto's own rule text rather than by a failed
 * tool call halfway through a batch.
 */
record AgentTurn(List<Long> handledEventCursors, List<Command> commands) {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_COMMANDS = 16;
    private static final int MAX_RESPONSE_CHARS = 256 * 1024;
    /** The command that records a decision to take no protocol action; host-local. */
    private static final String ACK = "host-ack";
    /** The command whose sender and recipient the host pins on both sides. */
    private static final String MESSAGE = "delegation-message";
    /** How deep the prose contract expands nested request messages. */
    private static final int CONTRACT_DEPTH = 3;

    /** The delegation request message each tool carries, by tool name. */
    private static final Map<String, Descriptor> REQUESTS = Map.of(
            "delegation-accept", AcceptTaskRequest.getDescriptor(),
            "delegation-progress", ReportProgressRequest.getDescriptor(),
            "delegation-checkpoint", RecordCheckpointRequest.getDescriptor(),
            "delegation-candidate", SubmitCandidateRequest.getDescriptor(),
            MESSAGE, SendTaskMessageRequest.getDescriptor(),
            "delegation-offer", OfferTaskRequest.getDescriptor(),
            "delegation-review", ReviewCandidateRequest.getDescriptor(),
            "delegation-cancel", CancelTaskRequest.getDescriptor());

    private static final List<String> WORKER_TOOLS = List.of(
            ACK, "delegation-accept", MESSAGE,
            "delegation-progress", "delegation-checkpoint", "delegation-candidate");
    private static final List<String> COORDINATOR_TOOLS = List.of(
            ACK, "delegation-offer", MESSAGE,
            "delegation-review", "delegation-cancel");

    /** One validator per request type, so CEL programs are compiled once. */
    private static final Map<String, ProtoValidator> VALIDATORS = new ConcurrentHashMap<>();

    private static final ProtoJsonSchemaGenerator SCHEMAS = ProtoJsonSchemaGenerator.create();

    /** The rule readers the validator and the schema generator both run on. */
    private static final List<ValidationRuleSource> RULE_SOURCES =
            ValidationRuleSources.defaults();

    /** Rejects text after the JSON value, so a candidate object is the whole candidate. */
    private static final ObjectReader STRICT = MAPPER.reader()
            .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    record Command(String tool, ObjectNode arguments) {
        Command {
            arguments = arguments.deepCopy();
        }
    }
    /**
     * Reads the JSON value in a provider reply. Providers that join every message chunk of
     * a turn (the ACP provider) hand over the model's narration between tool calls as well
     * as its final answer, and some models wrap the answer in a Markdown fence. The reply
     * is taken as a whole when it parses; otherwise the turn is the last complete JSON
     * object in it: the earliest opening brace from which the text up to the final closing
     * brace parses with no trailing tokens. An earlier, abandoned object followed by more
     * text therefore never wins over the answer that ends the reply.
     */
    static JsonNode readResponse(String text) {
        String trimmed = text.strip();
        JsonProcessingException whole;
        try {
            return STRICT.readTree(trimmed);
        } catch (JsonProcessingException e) {
            whole = e;
        }
        String completed = closeTrailingOpeners(trimmed);
        if (completed != null) {
            try {
                return STRICT.readTree(completed);
            } catch (JsonProcessingException incomplete) {
                // the missing closers were not the only defect
            }
        }
        int end = trimmed.lastIndexOf('}');
        int start = trimmed.indexOf('{');
        while (start >= 0 && start < end) {
            try {
                JsonNode candidate = STRICT.readTree(trimmed.substring(start, end + 1));
                if (candidate.isObject() && candidate.has("handledEventCursors")) {
                    return candidate;
                }
            } catch (JsonProcessingException partial) {
                // not a complete object from this brace; try the next one
            }
            start = trimmed.indexOf('{', start + 1);
        }
        throw new AgentHostException("agent response is not JSON: "
                + whole.getOriginalMessage() + " at line " + whole.getLocation().getLineNr()
                + " column " + whole.getLocation().getColumnNr());
    }

    /**
     * Returns the text with the closers its unclosed objects and arrays still need appended,
     * or null when nothing is open at the end or the text ends inside a string. Models
     * regularly stop one brace short of a well-formed reply; nothing else is repaired.
     */
    private static String closeTrailingOpeners(String text) {
        StringBuilder closers = new StringBuilder();
        boolean inString = false;
        boolean escaped = false;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (inString) {
                if (escaped) {
                    escaped = false;
                } else if (c == '\\') {
                    escaped = true;
                } else if (c == '"') {
                    inString = false;
                }
                continue;
            }
            switch (c) {
                case '"' -> inString = true;
                case '{' -> closers.append('}');
                case '[' -> closers.append(']');
                case '}', ']' -> {
                    if (closers.isEmpty() || closers.charAt(closers.length() - 1) != c) {
                        return null;
                    }
                    closers.setLength(closers.length() - 1);
                }
                default -> {
                    // other characters do not affect nesting
                }
            }
        }
        if (inString || closers.isEmpty()) {
            return null;
        }
        return text + closers.reverse();
    }

    static AgentTurn parse(String text, AgentRole role, List<Long> expectedCursors,
                           String identity) {
        if (text == null || text.length() > MAX_RESPONSE_CHARS) {
            throw new AgentHostException("agent response exceeds the 256 KiB limit");
        }
        JsonNode parsed = readResponse(text);
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
        List<String> allowed = tools(role);
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
                        + role.name().toLowerCase(Locale.ROOT));
            }
            ObjectNode arguments = ((ObjectNode) node.path("arguments")).deepCopy();
            if (ACK.equals(tool)) {
                validateAck(arguments);
            } else {
                enforceIdentity(role, identity, tool, arguments);
                validateRequest(tool, arguments, i);
            }
            commands.add(new Command(tool, arguments));
        }
        return new AgentTurn(List.copyOf(cursors), List.copyOf(commands));
    }

    /** The commands a role may issue, host-local acknowledgement first. */
    static List<String> tools(AgentRole role) {
        return role == AgentRole.WORKER ? WORKER_TOOLS : COORDINATOR_TOOLS;
    }

    /**
     * Refuses a command its delegation request message does not accept.
     *
     * <p>The arguments are the request message's canonical proto3 JSON, so the message is
     * what says whether they are a request at all: an unknown member is a field the caller
     * did not mean, and the declared rules are the ones the coordinator applies when the
     * call arrives. Running them here costs one parse and turns a mid-batch tool failure
     * into a repair turn that quotes the rule the model broke.
     */
    private static void validateRequest(String tool, ObjectNode arguments, int index) {
        Descriptor descriptor = REQUESTS.get(tool);
        String where = "command " + index + " " + tool;
        refuseUnknownMembers(arguments, descriptor, where);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        try {
            JsonFormat.parser().merge(arguments.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new AgentHostException(where + " is not a " + descriptor.getName()
                    + ": " + e.getMessage());
        }
        Message request = builder.build();
        ValidationResult result = VALIDATORS
                .computeIfAbsent(descriptor.getFullName(),
                        name -> ProtoValidator.forMessageType(descriptor))
                .validate(request);
        if (result.valid()) {
            return;
        }
        StringBuilder out = new StringBuilder(where).append(':');
        for (ValidationResult.Violation violation : result.violations()) {
            out.append(' ').append(jsonPath(violation.path())).append(' ')
                    .append(violation.message()).append(';');
        }
        out.setLength(out.length() - 1);
        throw new AgentHostException(out.toString());
    }

    /**
     * Refuses a member no field of the message declares, naming it.
     *
     * <p>The parser refuses unknown members too, but names them by the proto type they were
     * not found in; the model reads the tool name and the member it wrote.
     */
    private static void refuseUnknownMembers(JsonNode node, Descriptor descriptor,
                                             String where) {
        if (!node.isObject() || descriptor.getFullName().startsWith("google.protobuf.")) {
            return;
        }
        for (Map.Entry<String, JsonNode> member : node.properties()) {
            FieldDescriptor field = member(descriptor, member.getKey());
            if (field == null) {
                throw new AgentHostException(where + " contains unknown field '"
                        + member.getKey() + "'");
            }
            if (field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                    || field.isMapField()) {
                continue;
            }
            JsonNode value = member.getValue();
            if (field.isRepeated() && value.isArray()) {
                for (JsonNode element : value) {
                    refuseUnknownMembers(element, field.getMessageType(), where);
                }
            } else {
                refuseUnknownMembers(value, field.getMessageType(), where);
            }
        }
    }

    /** The field a member names, by its JSON spelling first and its declared name second. */
    private static FieldDescriptor member(Descriptor descriptor, String name) {
        for (FieldDescriptor field : descriptor.getFields()) {
            if (field.getJsonName().equals(name)) {
                return field;
            }
        }
        return descriptor.findFieldByName(name);
    }

    /** A validator path with each field named as the model wrote it in the arguments. */
    private static String jsonPath(String path) {
        StringBuilder out = new StringBuilder();
        for (String segment : path.split("\\.")) {
            if (out.length() > 0) {
                out.append('.');
            }
            int bracket = segment.indexOf('[');
            String name = bracket < 0 ? segment : segment.substring(0, bracket);
            boolean capitalize = false;
            for (int i = 0; i < name.length(); i++) {
                char character = name.charAt(i);
                if (character == '_') {
                    capitalize = true;
                } else if (capitalize) {
                    out.append(Character.toUpperCase(character));
                    capitalize = false;
                } else {
                    out.append(character);
                }
            }
            if (bracket >= 0) {
                out.append(segment.substring(bracket));
            }
        }
        return out.toString();
    }

    /**
     * The structured-output schema for a role: one variant per allowed command, each
     * wrapping the request message's own schema.
     *
     * <p>Rendered from the request descriptors, so the shape offered to a provider and the
     * rules applied to what comes back are the same statement. The identity members the
     * host sets itself are dropped from what the model is asked for.
     *
     * <p>Every object is closed and names every member it accepts in {@code required}: a
     * provider running strict structured output demands that, and a member the message
     * declares optional is offered as nullable rather than left out, because proto3 JSON
     * reads a null as the field being absent.
     */
    static ObjectNode outputSchema(AgentRole role) {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("handledEventCursors").put("type", "array")
                .putObject("items").put("type", "integer");
        ObjectNode commands = properties.putObject("commands");
        commands.put("type", "array").put("minItems", 1).put("maxItems", MAX_COMMANDS);
        ObjectNode defs = MAPPER.createObjectNode();
        ArrayNode variants = commands.putObject("items").putArray("anyOf");
        for (String tool : tools(role)) {
            variants.add(commandSchema(role, tool, defs));
        }
        schema.putArray("required").add("handledEventCursors").add("commands");
        schema.put("additionalProperties", false);
        ObjectNode reachable = reachableDefs(variants, defs);
        if (!reachable.isEmpty()) {
            schema.set("$defs", reachable);
        }
        return schema;
    }

    private static ObjectNode commandSchema(AgentRole role, String tool, ObjectNode defs) {
        ObjectNode variant = MAPPER.createObjectNode();
        variant.put("type", "object");
        ObjectNode properties = variant.putObject("properties");
        properties.putObject("tool").put("type", "string").put("const", tool);
        properties.set("arguments", ACK.equals(tool)
                ? ackSchema() : requestSchema(role, tool, defs));
        variant.putArray("required").add("tool").add("arguments");
        variant.put("additionalProperties", false);
        return variant;
    }

    /** The host's own acknowledgement, which no delegation verb carries. */
    private static ObjectNode ackSchema() {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        schema.putObject("properties").putObject("reason").put("type", "string")
                .put("minLength", 1).put("maxLength", 1_024);
        schema.putArray("required").add("reason");
        schema.put("additionalProperties", false);
        return schema;
    }

    private static ObjectNode requestSchema(AgentRole role, String tool, ObjectNode defs) {
        ObjectNode rendered = MAPPER.valueToTree(
                SCHEMAS.generateRooted(REQUESTS.get(tool)));
        rendered.remove("$schema");
        JsonNode nested = rendered.remove("$defs");
        if (nested != null) {
            for (Map.Entry<String, JsonNode> def : nested.properties()) {
                if (!defs.has(def.getKey())) {
                    harden(def.getValue());
                    defs.set(def.getKey(), def.getValue());
                }
            }
        }
        Set<String> pinned = pinnedFields(role, tool);
        JsonNode properties = rendered.path("properties");
        JsonNode required = rendered.path("required");
        for (String field : pinned) {
            if (properties.isObject()) {
                ((ObjectNode) properties).remove(field);
            }
            if (required.isArray()) {
                for (int i = 0; i < required.size(); i++) {
                    if (field.equals(required.get(i).asText())) {
                        ((ArrayNode) required).remove(i);
                        break;
                    }
                }
            }
        }
        harden(rendered);
        return rendered;
    }

    /**
     * Closes an object schema, names every member in {@code required}, and offers the
     * members the message does not require as nullable.
     */
    private static void harden(JsonNode node) {
        if (!node.isObject()) {
            return;
        }
        ObjectNode object = (ObjectNode) node;
        JsonNode declared = object.get("properties");
        if (declared instanceof ObjectNode members) {
            Set<String> required = new LinkedHashSet<>();
            for (JsonNode entry : object.path("required")) {
                required.add(entry.asText());
            }
            ArrayNode names = MAPPER.createArrayNode();
            for (Map.Entry<String, JsonNode> member : List.copyOf(members.properties())) {
                names.add(member.getKey());
                if (!required.contains(member.getKey())) {
                    members.set(member.getKey(), nullable(member.getValue()));
                }
            }
            object.set("required", names);
            object.put("additionalProperties", false);
            members.properties().forEach(member -> harden(member.getValue()));
        }
        harden(object.path("items"));
        harden(object.path("additionalProperties"));
        for (JsonNode branch : object.path("anyOf")) {
            harden(branch);
        }
        for (JsonNode branch : object.path("allOf")) {
            harden(branch);
        }
    }

    /** The same schema, with a null alternative for a member the message does not require. */
    private static JsonNode nullable(JsonNode schema) {
        if (schema.isObject() && schema.get("anyOf") instanceof ArrayNode branches) {
            branches.addObject().put("type", "null");
            return schema;
        }
        ObjectNode wrapper = MAPPER.createObjectNode();
        ArrayNode branches = wrapper.putArray("anyOf");
        branches.add(schema);
        branches.addObject().put("type", "null");
        return wrapper;
    }

    /**
     * The definitions the command variants actually reach. Each rendered request carries
     * every type it can see, its own included; a schema that states types nothing refers
     * to is describing messages the model is not being asked for.
     */
    private static ObjectNode reachableDefs(JsonNode variants, ObjectNode defs) {
        ObjectNode kept = MAPPER.createObjectNode();
        Deque<String> pending = new ArrayDeque<>(references(variants));
        while (!pending.isEmpty()) {
            String name = pending.poll();
            JsonNode def = defs.get(name);
            if (def == null || kept.has(name)) {
                continue;
            }
            kept.set(name, def);
            pending.addAll(references(def));
        }
        return kept;
    }

    private static Set<String> references(JsonNode node) {
        Set<String> found = new LinkedHashSet<>();
        collectReferences(node, found);
        return found;
    }

    private static void collectReferences(JsonNode node, Set<String> found) {
        if (node.isObject()) {
            JsonNode reference = node.get("$ref");
            if (reference != null && reference.isTextual()
                    && reference.asText().startsWith("#/$defs/")) {
                found.add(reference.asText().substring("#/$defs/".length()));
            }
            node.properties().forEach(member -> collectReferences(member.getValue(), found));
        } else if (node.isArray()) {
            for (JsonNode element : node) {
                collectReferences(element, found);
            }
        }
    }

    /**
     * The argument contract for a role in prose, rendered from the same request
     * descriptors the schema and the validator read.
     *
     * <p>The prompt states the shape a command takes and the rules a message declares
     * about itself; both come from the descriptor, so neither can drift from what the
     * host will accept.
     */
    static String commandContract(AgentRole role) {
        StringBuilder out = new StringBuilder(role == AgentRole.WORKER
                ? "Worker argument contract, from the delegation request messages: "
                : "Coordinator argument contract, from the delegation request messages: ");
        Set<String> rules = new LinkedHashSet<>();
        boolean first = true;
        for (String tool : tools(role)) {
            if (!first) {
                out.append("; ");
            }
            first = false;
            out.append(tool).append('=');
            if (ACK.equals(tool)) {
                out.append("{reason}");
            } else {
                out.append(shape(REQUESTS.get(tool), pinnedFields(role, tool),
                        new HashSet<>(), 0, rules));
            }
        }
        out.append('.');
        if (!rules.isEmpty()) {
            out.append(" Rules the messages declare: ")
                    .append(String.join("; ", rules)).append('.');
        }
        return out.append(" Use exactly these field names and no others.").toString();
    }

    /** One message as its member names, expanding the messages it carries. */
    private static String shape(Descriptor descriptor, Set<String> omit, Set<String> seen,
                                int depth, Set<String> rules) {
        rules.addAll(messageRules(descriptor));
        if (depth >= CONTRACT_DEPTH || !seen.add(descriptor.getFullName())) {
            return "{...}";
        }
        StringBuilder out = new StringBuilder("{");
        boolean first = true;
        for (FieldDescriptor field : descriptor.getFields()) {
            String name = field.getJsonName();
            if (depth == 0 && omit.contains(name)) {
                continue;
            }
            if (!first) {
                out.append(',');
            }
            first = false;
            out.append(name);
            String nested = "";
            if (field.getJavaType() == FieldDescriptor.JavaType.ENUM) {
                nested = choices(field);
            } else if (field.getJavaType() == FieldDescriptor.JavaType.MESSAGE
                    && !field.isMapField()
                    && !field.getMessageType().getFullName().startsWith("google.protobuf.")) {
                nested = shape(field.getMessageType(), Set.of(), seen, depth + 1, rules);
            }
            out.append(field.isRepeated() ? "[" + nested + "]" : nested);
        }
        seen.remove(descriptor.getFullName());
        return out.append('}').toString();
    }

    /** An enum field's declared values, less the unspecified zero no request may send. */
    private static String choices(FieldDescriptor field) {
        List<String> names = new ArrayList<>();
        field.getEnumType().getValues().stream()
                .filter(value -> value.getNumber() != 0)
                .forEach(value -> names.add(value.getName()));
        return "(" + String.join("|", names) + ")";
    }

    /** What a message states about itself as a whole, in the words of its own rules. */
    private static List<String> messageRules(Descriptor descriptor) {
        List<String> stated = new ArrayList<>();
        for (ValidationRuleSource source : RULE_SOURCES) {
            MessageConstraints constraints = source.messageConstraints(descriptor)
                    .orElse(null);
            if (constraints == null) {
                continue;
            }
            for (CelConstraint rule : constraints.cel()) {
                if (!rule.message().isBlank()) {
                    stated.add(rule.message());
                }
            }
        }
        return stated;
    }

    /** The members the host sets itself, which the model is neither asked for nor allowed. */
    private static Set<String> pinnedFields(AgentRole role, String tool) {
        if (ACK.equals(tool)) {
            return Set.of();
        }
        if (role == AgentRole.WORKER) {
            return MESSAGE.equals(tool) ? Set.of("sender", "recipient") : Set.of("workerId");
        }
        return MESSAGE.equals(tool) ? Set.of("sender") : Set.of();
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
        if (role == AgentRole.WORKER) {
            if (MESSAGE.equals(tool)) {
                requireCompatible(arguments, "sender", identity);
                requireCompatible(arguments, "recipient", "coordinator");
                arguments.put("sender", identity);
                arguments.put("recipient", "coordinator");
            } else {
                requireCompatible(arguments, "workerId", identity);
                arguments.put("workerId", identity);
            }
        } else if (MESSAGE.equals(tool)) {
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
}
