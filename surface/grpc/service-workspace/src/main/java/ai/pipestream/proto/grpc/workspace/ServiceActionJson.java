package ai.pipestream.proto.grpc.workspace;

import ai.pipestream.proto.actions.ActionException;
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
import com.google.protobuf.Parser;
import com.google.protobuf.util.JsonFormat;

/**
 * Shared contract plumbing for the service-workspace verbs: the request message each verb is
 * declared in, its derived input schema, and envelope parsing that enforces the declared
 * rules.
 *
 * <p>The service definition is compiled from source at load and bound with dynamic messages,
 * so a request message is reached by name off that descriptor rather than through a generated
 * class.
 */
final class ServiceActionJson {

    /**
     * Enforces the request contract on the catalog path.
     *
     * <p>Calls arriving over gRPC pass a validating interceptor before they reach a handler.
     * Calls arriving as catalog verbs do not, so without this the same request would be
     * refused on one surface and accepted on the other.
     */
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ServiceActionJson() {
    }

    /** The request message a verb is declared in, by its name in the service definition. */
    static Descriptor request(String message) {
        Descriptor descriptor = ProtoMoltServiceSchema.file().findMessageTypeByName(message);
        if (descriptor == null) {
            throw new IllegalStateException(
                    "The service definition declares no message named " + message);
        }
        return descriptor;
    }

    /**
     * The input schema for a verb, derived from the request message it accepts.
     *
     * <p>Deriving rather than hand-writing keeps one description of the contract. The
     * generator folds the message's declared validation rules into the schema, so a caller
     * reading the tool manifest sees the same bounds the verb enforces, and a rule added to
     * the proto reaches every surface without a second edit.
     */
    static ObjectNode schemaFor(String message) {
        return MAPPER.valueToTree(ProtoJsonSchemaGenerator.create().generateRooted(request(message)));
    }

    /**
     * Parses an action envelope into the request message it must satisfy.
     *
     * <p>The envelope is the message's canonical proto3 JSON form, so the same document works
     * over the catalog, over the JSON gateway, and as a tool call. Unknown members are refused
     * rather than ignored: a caller that misspells a field has written a request it did not
     * mean, and silently dropping it would do something else.
     */
    static DynamicMessage parse(ObjectNode input, String message, String verb)
            throws ActionException {
        Descriptor descriptor = request(message);
        DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);
        try {
            JsonFormat.parser().merge(input.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("invalid-input",
                    verb + " expects a " + descriptor.getName() + ": " + e.getMessage());
        }
        DynamicMessage parsed = builder.build();
        ValidationResult result = VALIDATOR.validate(parsed);
        if (!result.valid()) {
            throw new ActionException("invalid-input",
                    verb + " does not satisfy the request contract: " + describe(result),
                    violations(result));
        }
        return parsed;
    }

    /** The value of a declared string field on a parsed request. */
    static String string(DynamicMessage request, String field) {
        return (String) request.getField(request.getDescriptorForType().findFieldByName(field));
    }

    /**
     * The value of a declared int32 field, or {@code fallback} when it is zero.
     *
     * <p>proto3 cannot distinguish an absent number from a zero one, so the contract gives
     * zero the meaning "the action's default" and declares it in the field's comment. That
     * is why the rules admit zero rather than requiring a positive value.
     */
    static int number(DynamicMessage request, String field, int fallback) {
        int value = (Integer) request.getField(
                request.getDescriptorForType().findFieldByName(field));
        return value == 0 ? fallback : value;
    }

    /**
     * A declared message field re-read as its generated type.
     *
     * <p>The service definition is compiled at load, so a submessage arrives as a dynamic
     * message. Its descriptor is the same one the generated class was built from, so the
     * encoded bytes parse directly and the action works with the typed value.
     */
    static <T extends Message> T submessage(DynamicMessage request, String field,
            Parser<T> parser, String verb) throws ActionException {
        Message value = (Message) request.getField(
                request.getDescriptorForType().findFieldByName(field));
        try {
            return parser.parseFrom(value.toByteString());
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("invalid-input",
                    verb + " could not read '" + field + "': " + e.getMessage());
        }
    }

    /** The violations as machine-readable details, each naming its field and its rule. */
    private static ObjectNode violations(ValidationResult result) {
        ObjectNode details = MAPPER.createObjectNode();
        ArrayNode listed = details.putArray("violations");
        for (ValidationResult.Violation violation : result.violations()) {
            ObjectNode node = listed.addObject();
            node.put("field", violation.path());
            node.put("ruleId", violation.ruleId());
            node.put("message", violation.message());
        }
        return details;
    }

    /** The violations as one human-readable sentence, in declaration order. */
    private static String describe(ValidationResult result) {
        StringBuilder out = new StringBuilder();
        for (ValidationResult.Violation violation : result.violations()) {
            if (out.length() > 0) {
                out.append("; ");
            }
            out.append(violation.path()).append(' ').append(violation.message());
        }
        return out.toString();
    }
}
