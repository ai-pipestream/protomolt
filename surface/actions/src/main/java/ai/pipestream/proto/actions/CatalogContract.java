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
import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
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

    private static final JsonFormat.Printer COMPLETE_PRINTER =
            JsonFormat.printer().alwaysPrintFieldsWithNoPresence();

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

    /** The response message a verb answers with, by its name in the service definition. */
    public static Descriptor response(String message) {
        return request(message);
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
        toRequest(input, descriptor, verb);
    }

    /**
     * Parses an envelope into the request message a verb accepts, refusing what the message
     * does not allow.
     *
     * <p>The envelope is the message's canonical proto3 JSON form, so the same document works
     * over the catalog, over the JSON gateway, and as a tool call. Unknown members are refused
     * rather than ignored: a caller that misspells a field has written a request it did not
     * mean, and silently dropping it would do something else.
     *
     * <p>The parsed message is the value the verb runs on, so a front that arrives as JSON
     * converts once here and every surface hands the verb the same typed request.
     */
    public static Message toRequest(ObjectNode input, Descriptor descriptor, String verb)
            throws ActionException {
        EnvelopeTypes.check(input, descriptor, "");
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        try {
            JsonFormat.parser().merge(input.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("invalid-input",
                    verb + " expects a " + descriptor.getName() + ": " + e.getMessage());
        }
        DynamicMessage request = builder.build();
        validate(request, descriptor, verb);
        return request;
    }

    /**
     * Refuses a request the message's own rules do not allow.
     *
     * <p>The rules live on the request message, so they are the same rules whichever surface
     * the call arrived on. Checking them where dispatch happens rather than at each front is
     * what keeps a request from being refused over gRPC and accepted as a tool call.
     */
    public static void validate(Message request, Descriptor descriptor, String verb)
            throws ActionException {
        // Compared by name rather than by identity: the same contract reaches a verb both
        // as a generated message and as a dynamic one built off the compiled descriptor, and
        // those are the same type even though they are not the same descriptor instance.
        if (!request.getDescriptorForType().getFullName().equals(descriptor.getFullName())) {
            throw new ActionException("invalid-input",
                    verb + " expects a " + descriptor.getFullName() + ", not a "
                            + request.getDescriptorForType().getFullName());
        }
        ValidationResult result = VALIDATOR.validate(request);
        if (!result.valid()) {
            throw new ActionException("invalid-input",
                    verb + " does not satisfy the request contract: " + describe(result),
                    violations(result));
        }
    }

    /**
     * The request as the generated type a verb is written against.
     *
     * <p>A verb declared on a contract with generated stubs reads its request through them.
     * The same request reaches it as a generated message over gRPC and as a dynamic one from
     * a JSON front, and those are the same message in two representations, so the dynamic one
     * is re-read through the generated parser. A request that is already the right class is
     * handed straight back.
     *
     * @param prototype the default instance of the type the verb reads, e.g.
     *        {@code AcceptTaskRequest.getDefaultInstance()}
     */
    @SuppressWarnings("unchecked")
    public static <T extends Message> T as(Message request, T prototype, String verb)
            throws ActionException {
        if (prototype.getClass().isInstance(request)) {
            return (T) request;
        }
        Descriptor expected = prototype.getDescriptorForType();
        if (!request.getDescriptorForType().getFullName().equals(expected.getFullName())) {
            throw new ActionException("invalid-input",
                    verb + " expects a " + expected.getFullName() + ", not a "
                            + request.getDescriptorForType().getFullName());
        }
        try {
            return (T) prototype.getParserForType().parseFrom(request.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("internal-error",
                    verb + " request does not re-read as " + expected.getFullName()
                            + ": " + e.getMessage());
        }
    }

    /**
     * A request rendered as the JSON envelope a verb still written against JSON reads.
     *
     * <p>Absent fields stay absent. A verb reading this envelope distinguishes "not given"
     * from "given as the default" by whether the member is there, so printing defaults would
     * present every arm of an either-or as supplied at once.
     */
    public static ObjectNode toEnvelope(MessageOrBuilder message, String verb)
            throws ActionException {
        return print(JsonFormat.printer(), message, verb, "request");
    }

    /**
     * A response rendered as the JSON document a JSON caller reads.
     *
     * <p>Fields without presence are printed even at their default, so the reply has the
     * shape the response message declares rather than the shape one run happened to populate.
     * Without that, {@code ok: false} and an empty list of findings are both indistinguishable
     * from a field the verb forgot to write.
     */
    public static ObjectNode toReply(MessageOrBuilder message, String verb)
            throws ActionException {
        return print(COMPLETE_PRINTER, message, verb, "result");
    }

    private static ObjectNode print(JsonFormat.Printer printer, MessageOrBuilder message,
            String verb, String what) throws ActionException {
        try {
            String json = printer.print(message);
            return (ObjectNode) MAPPER.readTree(json.isBlank() ? "{}" : json);
        } catch (Exception e) {
            throw new ActionException("internal-error",
                    verb + " " + what + " does not render as JSON: " + e.getMessage());
        }
    }

    /**
     * Parses a verb's JSON result into the response message the service declares for it.
     *
     * <p>Unknown members are refused. A verb that writes a field its response message does
     * not declare has answered something other than its contract, and accepting the document
     * with that field dropped would report success for a reply the caller never receives.
     */
    public static Message toResponse(ObjectNode output, Descriptor descriptor, String verb)
            throws ActionException {
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        try {
            JsonFormat.parser().merge(output.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("internal-error",
                    "Result of " + verb + " does not parse as " + descriptor.getFullName()
                            + ": " + e.getMessage());
        }
        return builder.build();
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
