package ai.pipestream.proto.actions;

import ai.pipestream.proto.compat.Impact;
import ai.pipestream.proto.compat.SchemaChange;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.NullValue;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/** Shared JSON assembly for actions: input-schema fragments, change and CEL value rendering. */
final class ActionJson {

    /** Mapper for context-free {@link ProtoAction#inputSchema()} documents. */
    static final ObjectMapper SCHEMA_MAPPER = new ObjectMapper();

    private ActionJson() {
    }

    // ---- input schema building ----

    /** The 2020-12 envelope skeleton every action schema starts from. */
    static ObjectNode baseInputSchema() {
        ObjectNode schema = SCHEMA_MAPPER.createObjectNode();
        schema.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        schema.put("type", "object");
        return schema;
    }

    static void required(ObjectNode schema, String... fields) {
        ArrayNode required = schema.putArray("required");
        for (String field : fields) {
            required.add(field);
        }
    }

    /** The shared one-of schema-source fragment (type | sources+root | descriptorSetBase64). */
    static ObjectNode schemaSourceSchema() {
        ObjectNode schema = SCHEMA_MAPPER.createObjectNode();
        schema.put("type", "object");
        schema.put("description",
                "Schema source; provide exactly one of 'type', 'sources', 'descriptorSetBase64'.");
        ObjectNode properties = schema.putObject("properties");
        properties.putObject("type")
                .put("type", "string")
                .put("description",
                        "Fully qualified protobuf message name resolved from the shared descriptor registry, "
                                + "e.g. 'google.protobuf.Struct'. Discover names with the list-types action.");
        ObjectNode sources = properties.putObject("sources");
        sources.put("type", "object");
        sources.put("description",
                "Inline .proto files keyed by import path; compiled per call, never registered.");
        sources.putObject("additionalProperties").put("type", "string");
        properties.putObject("root")
                .put("type", "string")
                .put("description",
                        "Optional entry file within 'sources'; when it declares exactly one message, "
                                + "that message becomes the default 'type'.");
        properties.putObject("descriptorSetBase64")
                .put("type", "string")
                .put("contentEncoding", "base64")
                .put("description",
                        "Base64-encoded serialized google.protobuf.FileDescriptorSet, e.g. from the compile action.");
        ArrayNode oneOf = schema.putArray("oneOf");
        oneOf.addObject().putArray("required").add("type");
        oneOf.addObject().putArray("required").add("sources");
        oneOf.addObject().putArray("required").add("descriptorSetBase64");
        return schema;
    }

    static ObjectNode typeProperty(String description) {
        return SCHEMA_MAPPER.createObjectNode()
                .put("type", "string")
                .put("description", description);
    }

    // ---- result rendering ----

    static ObjectNode change(SchemaChange change, ObjectMapper mapper) {
        ObjectNode node = mapper.createObjectNode();
        node.put("ruleId", change.ruleId());
        node.put("path", change.path());
        node.put("before", change.before());
        node.put("after", change.after());
        node.put("message", change.message());
        ArrayNode impacts = node.putArray("impacts");
        change.impacts().stream().map(Impact::name).sorted().forEach(impacts::add);
        return node;
    }

    /** A protobuf message as a Jackson tree, via canonical proto3 JSON. */
    static JsonNode messageToJson(Message message, ActionContext context) throws ActionException {
        try {
            return context.objectMapper().readTree(context.transcoder().toJson(message));
        } catch (JsonProcessingException e) {
            throw new ActionException("internal-error",
                    "Failed to render protobuf message as JSON: " + e.getMessage());
        }
    }

    /** A CEL evaluation result as a JSON value. */
    static JsonNode celValue(Object value, ActionContext context) throws ActionException {
        ObjectMapper mapper = context.objectMapper();
        switch (value) {
            case null -> {
                return NullNode.getInstance();
            }
            case NullValue ignored -> {
                return NullNode.getInstance();
            }
            case Boolean b -> {
                return mapper.getNodeFactory().booleanNode(b);
            }
            case String s -> {
                return mapper.getNodeFactory().textNode(s);
            }
            case ByteString bytes -> {
                return mapper.getNodeFactory()
                        .textNode(Base64.getEncoder().encodeToString(bytes.toByteArray()));
            }
            case Double d -> {
                return mapper.getNodeFactory().numberNode(d);
            }
            case Float f -> {
                return mapper.getNodeFactory().numberNode(f);
            }
            case Long l -> {
                return mapper.getNodeFactory().numberNode(l);
            }
            case Integer i -> {
                return mapper.getNodeFactory().numberNode(i);
            }
            case Message message -> {
                return messageToJson(message, context);
            }
            case List<?> list -> {
                ArrayNode array = mapper.createArrayNode();
                for (Object element : list) {
                    array.add(celValue(element, context));
                }
                return array;
            }
            case Map<?, ?> map -> {
                ObjectNode object = mapper.createObjectNode();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    object.set(String.valueOf(entry.getKey()), celValue(entry.getValue(), context));
                }
                return object;
            }
            // CEL uint results (guava UnsignedLong) and any other numeric wrapper
            case Number n -> {
                return mapper.getNodeFactory().numberNode(n.longValue());
            }
            default -> {
                return mapper.getNodeFactory().textNode(String.valueOf(value));
            }
        }
    }

    /** A short, stable type label for a CEL evaluation result. */
    static String celType(Object value) {
        return switch (value) {
            case null -> "null";
            case NullValue ignored -> "null";
            case Boolean ignored -> "bool";
            case String ignored -> "string";
            case ByteString ignored -> "bytes";
            case Double ignored -> "double";
            case Float ignored -> "double";
            case Long ignored -> "int";
            case Integer ignored -> "int";
            case Message message -> "message:" + message.getDescriptorForType().getFullName();
            case List<?> ignored -> "list";
            case Map<?, ?> ignored -> "map";
            case Number ignored -> "uint";
            default -> value.getClass().getSimpleName();
        };
    }
}
