package ai.protomolt.proto.agenthost;

import ai.protomolt.proto.delegation.contract.DeliverableContracts;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Renders the structured-output schema for a role from the command catalog's
 * request descriptors. Operates on descriptors and deliverable contracts
 * only: nothing here sees credentials, provider processes or session state.
 */
final class AgentTurnSchemas {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TYPE_URL_PREFIX = "type.googleapis.com/";
    private static final String CANDIDATE_TYPE =
            "ai.protomolt.proto.delegation.v1.CompletionCandidate";

    private static final ProtoJsonSchemaGenerator SCHEMAS = ProtoJsonSchemaGenerator.create();

    private AgentTurnSchemas() {
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
    static ObjectNode outputSchema(AgentRole role, Map<String, DeliverableContract> contracts) {
        ObjectNode schema = MAPPER.createObjectNode();
        schema.put("type", "object");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("handledEventCursors").put("type", "array")
                .putObject("items").put("type", "integer");
        ObjectNode commands = properties.putObject("commands");
        commands.put("type", "array").put("minItems", 1)
                .put("maxItems", AgentCommands.MAX_COMMANDS);
        ObjectNode defs = MAPPER.createObjectNode();
        ArrayNode variants = commands.putObject("items").putArray("anyOf");
        for (String tool : AgentCommands.tools(role)) {
            variants.add(commandSchema(role, tool, defs));
        }
        schema.putArray("required").add("handledEventCursors").add("commands");
        schema.put("additionalProperties", false);
        renderDeliverable(defs, contracts);
        ObjectNode reachable = reachableDefs(variants, defs);
        if (!reachable.isEmpty()) {
            schema.set("$defs", reachable);
        }
        return schema;
    }

    /**
     * {@code CompletionCandidate.result} is an Any, which renders as an open object that a
     * strict structured-output endpoint refuses. With no contract known the member is left
     * out; with contracts known it becomes one closed branch per contract type, rendered
     * from the contract's descriptor with the type URL pinned, plus null for a task that
     * has no contract.
     */
    private static void renderDeliverable(ObjectNode defs,
                                          Map<String, DeliverableContract> contracts) {
        JsonNode candidate = defs.get(CANDIDATE_TYPE);
        if (!(candidate instanceof ObjectNode def)) {
            return;
        }
        ObjectNode properties = (ObjectNode) def.path("properties");
        ArrayNode required = (ArrayNode) def.path("required");
        Map<String, DeliverableContract> distinctContracts = new LinkedHashMap<>();
        for (DeliverableContract contract : contracts.values()) {
            distinctContracts.putIfAbsent(contract.getTypeName() + ":" + Base64.getEncoder()
                    .encodeToString(contract.getDescriptorSet().toByteArray()), contract);
        }
        if (distinctContracts.isEmpty()) {
            properties.remove("result");
            for (int i = 0; i < required.size(); i++) {
                if ("result".equals(required.get(i).asText())) {
                    required.remove(i);
                    break;
                }
            }
            return;
        }
        ObjectNode result = MAPPER.createObjectNode();
        ArrayNode branches = result.putArray("anyOf");
        List<String> identities = new ArrayList<>(distinctContracts.keySet());
        identities.sort(String::compareTo);
        for (int index = 0; index < identities.size(); index++) {
            DeliverableContract contract = distinctContracts.get(identities.get(index));
            Descriptor type;
            try {
                type = DeliverableContracts.compile(contract).descriptor();
            } catch (IllegalArgumentException unlinkable) {
                continue;
            }
            ObjectNode rendered = MAPPER.valueToTree(SCHEMAS.generateRooted(type));
            rendered.remove("$schema");
            JsonNode nested = rendered.path("$defs");
            if (nested.isObject()) {
                String namespace = "deliverable-" + index + "::";
                Map<String, String> renamed = new LinkedHashMap<>();
                nested.fieldNames().forEachRemaining(name ->
                        renamed.put(name, namespace + name));
                rewriteLocalRefs(rendered, renamed);
                rendered.remove("$defs");
                for (Map.Entry<String, JsonNode> entry : nested.properties()) {
                    harden(entry.getValue());
                    defs.set(renamed.get(entry.getKey()), entry.getValue());
                }
            }
            ObjectNode members = rendered.withObject("properties");
            ObjectNode typeUrl = members.putObject("@type");
            typeUrl.put("type", "string");
            typeUrl.putArray("enum").add(TYPE_URL_PREFIX + contract.getTypeName());
            rendered.withArray("required").add("@type");
            harden(rendered);
            branches.add(rendered);
        }
        branches.addObject().put("type", "null");
        properties.set("result", result);
    }

    /** Keeps every local reference with its contract, including recursive references. */
    private static void rewriteLocalRefs(JsonNode node, Map<String, String> renamed) {
        if (node instanceof ObjectNode object) {
            JsonNode ref = object.get("$ref");
            if (ref != null && ref.isTextual()) {
                String value = ref.asText();
                String original = value.startsWith("#/$defs/")
                        ? value.substring("#/$defs/".length()) : "";
                String replacement = renamed.get(original);
                if (replacement != null) {
                    object.put("$ref", "#/$defs/" + replacement);
                }
            }
            object.properties().forEach(entry -> rewriteLocalRefs(entry.getValue(), renamed));
        } else if (node instanceof ArrayNode array) {
            array.forEach(child -> rewriteLocalRefs(child, renamed));
        }
    }

    private static ObjectNode commandSchema(AgentRole role, String tool, ObjectNode defs) {
        ObjectNode variant = MAPPER.createObjectNode();
        variant.put("type", "object");
        ObjectNode properties = variant.putObject("properties");
        properties.putObject("tool").put("type", "string").put("const", tool);
        properties.set("arguments", AgentCommands.ACK.equals(tool)
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
                SCHEMAS.generateRooted(AgentCommands.REQUESTS.get(tool)));
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
        Set<String> pinned = AgentCommands.pinnedFields(role, tool);
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
     * Closes an object schema, names every member in {@code required}, offers the members
     * the message does not require as nullable, and drops vendor keywords.
     */
    private static void harden(JsonNode node) {
        if (!node.isObject()) {
            return;
        }
        ObjectNode object = (ObjectNode) node;
        // Strict structured-output endpoints accept standard keywords only; the rule text
        // behind the vendor keywords reaches the model through the prompt instead.
        List<String> vendor = new ArrayList<>();
        object.fieldNames().forEachRemaining(name -> {
            if (name.startsWith("x-")) {
                vendor.add(name);
            }
        });
        vendor.forEach(object::remove);
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
}
