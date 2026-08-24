package ai.pipestream.proto.actions;

import ai.pipestream.proto.grpc.service.contract.ProtoMoltServiceSchema;
import ai.pipestream.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

/**
 * The declared contract behind a catalog verb: the request message it accepts, the input
 * schema derived from that message, and envelope parsing that enforces the message's rules.
 *
 * <p>Every verb is declared as an RPC on the ProtoMolt service, so the request message is
 * the one description of what the verb takes. Deriving the published schema from it means a
 * caller reading the tool manifest sees the bounds the verb applies, and a rule added to the
 * proto reaches every surface without a second edit.
 *
 * <p>The definition is compiled from source at load, so a request message is reached by name
 * off that descriptor rather than through a generated class.
 */
public final class CatalogContract {

    /**
     * Enforces the request contract on the catalog path.
     *
     * <p>Calls arriving over gRPC pass a validating interceptor before they reach a handler.
     * Calls arriving as catalog verbs do not, so without this the same request would be
     * refused on one surface and accepted on the other.
     */
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private CatalogContract() {
    }

    /** The request message a verb is declared in, by its name in the service definition. */
    public static Descriptor request(String message) {
        Descriptor descriptor = ProtoMoltServiceSchema.file().findMessageTypeByName(message);
        if (descriptor == null) {
            throw new IllegalStateException(
                    "The service definition declares no message named " + message);
        }
        return descriptor;
    }

    /** The input schema for a verb, derived from the request message it accepts. */
    public static ObjectNode schemaFor(String message) {
        return schemaFor(request(message));
    }

    /** The input schema derived from a request message. */
    public static ObjectNode schemaFor(Descriptor request) {
        return MAPPER.valueToTree(ProtoJsonSchemaGenerator.create().generateRooted(request));
    }

    /**
     * The schema with {@code field} added to its required list.
     *
     * <p>For a field whose necessity depends on how the node is configured rather than on
     * the message: the contract cannot state it, because the same message is correct with
     * and without the field depending on what the node has to fall back on. The verb knows
     * which case it is in and says so in what it publishes.
     */
    public static ObjectNode requiring(ObjectNode schema, String field) {
        ArrayNode required = schema.has("required")
                ? (ArrayNode) schema.get("required")
                : schema.putArray("required");
        for (var element : required) {
            if (field.equals(element.asText())) {
                return schema;
            }
        }
        required.add(field);
        return schema;
    }

    /**
     * Refuses an envelope the request message does not accept.
     *
     * <p>The envelope is the message's canonical proto3 JSON form, so the same document works
     * over the catalog, over the JSON gateway, and as a tool call. Unknown members are refused
     * rather than ignored: a caller that misspells a field has written a request it did not
     * mean, and silently dropping it would do something else.
     */
    public static void check(ObjectNode input, String message, String verb)
            throws ActionException {
        check(input, request(message), verb);
    }

    /**
     * Refuses an envelope the request message does not accept.
     *
     * <p>The envelope is the message's canonical proto3 JSON form, so the same document works
     * over the catalog, over the JSON gateway, and as a tool call. Unknown members are refused
     * rather than ignored: a caller that misspells a field has written a request it did not
     * mean, and silently dropping it would do something else.
     */
    public static void check(ObjectNode input, Descriptor descriptor, String verb)
            throws ActionException {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        try {
            JsonFormat.parser().merge(input.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("invalid-input",
                    verb + " expects a " + descriptor.getName() + ": " + e.getMessage());
        }
        ValidationResult result = VALIDATOR.validate(builder.build());
        if (!result.valid()) {
            throw new ActionException("invalid-input",
                    verb + " does not satisfy the request contract: " + describe(result),
                    violations(result));
        }
    }

    /**
     * The violations as machine-readable details.
     *
     * <p>Carries the {@code pointer} the catalog's error contract has always reported, so a
     * caller that located a bad field from the pointer keeps doing so now that the refusal
     * comes from the declared rules. The validator names a field by its proto path; the
     * pointer names it as it appears in the JSON envelope the caller actually sent.
     */
    private static ObjectNode violations(ValidationResult result) {
        ObjectNode details = MAPPER.createObjectNode();
        result.violations().stream().findFirst().ifPresent(
                first -> details.put("pointer", pointer(first.path())));
        ArrayNode listed = details.putArray("violations");
        for (ValidationResult.Violation violation : result.violations()) {
            ObjectNode node = listed.addObject();
            node.put("field", jsonPath(violation.path()));
            node.put("ruleId", violation.ruleId());
            node.put("message", violation.message());
        }
        return details;
    }

    /**
     * A validator path rendered as a JSON Pointer into the envelope: dotted proto field
     * names become slash-separated JSON names, which is how the caller wrote them, and a
     * repeated element's bracketed index becomes its own segment, as RFC 6901 requires.
     */
    private static String pointer(String path) {
        StringBuilder out = new StringBuilder();
        for (String segment : jsonPath(path).split("\\.")) {
            out.append('/').append(segment.replace("[", "/").replace("]", ""));
        }
        return out.toString();
    }

    /** A validator path with each field named as the caller wrote it in the envelope. */
    private static String jsonPath(String path) {
        StringBuilder out = new StringBuilder();
        for (String segment : path.split("\\.")) {
            if (out.length() > 0) {
                out.append('.');
            }
            int bracket = segment.indexOf('[');
            out.append(bracket < 0
                    ? jsonName(segment)
                    : jsonName(segment.substring(0, bracket)) + segment.substring(bracket));
        }
        return out.toString();
    }

    /** One proto field name as its proto3 JSON spelling. */
    private static String jsonName(String field) {
        StringBuilder out = new StringBuilder(field.length());
        boolean capitalize = false;
        for (int i = 0; i < field.length(); i++) {
            char character = field.charAt(i);
            if (character == '_') {
                capitalize = true;
            } else if (capitalize) {
                out.append(Character.toUpperCase(character));
                capitalize = false;
            } else {
                out.append(character);
            }
        }
        return out.toString();
    }

    /** The violations as one human-readable sentence, in declaration order. */
    private static String describe(ValidationResult result) {
        StringBuilder out = new StringBuilder();
        for (ValidationResult.Violation violation : result.violations()) {
            if (out.length() > 0) {
                out.append("; ");
            }
            out.append(jsonPath(violation.path())).append(' ').append(violation.message());
        }
        return out.toString();
    }
}
