package ai.pipestream.proto.delegation;

import ai.pipestream.proto.actions.ActionContext;
import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.http.jsonschema.ProtoJsonSchemaGenerator;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

/**
 * Shared contract plumbing for the delegation catalog actions: derived input schemas,
 * envelope parsing against the declared request message, proto3 JSON rendering, and the
 * stable error codes the tools report.
 */
final class DelegationActionJson {

    /**
     * Enforces the request contract on the catalog path.
     *
     * <p>Calls arriving over gRPC pass a validating interceptor before they reach a handler.
     * Calls arriving as catalog verbs do not, so without this the same request would be
     * refused on one surface and accepted on the other.
     */
    private static final ProtoValidator VALIDATOR = ProtoValidator.create();

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private DelegationActionJson() {
    }

    /**
     * The input schema for a verb, derived from the request message it accepts.
     *
     * <p>Deriving rather than hand-writing keeps one description of the contract. The generator
     * folds the message's declared validation rules into the schema, so a caller reading the
     * tool manifest sees the same bounds the verb enforces, and a rule added to the proto
     * reaches every surface without a second edit.
     */
    static ObjectNode schemaFor(Descriptor request) {
        return MAPPER.valueToTree(ProtoJsonSchemaGenerator.create().generateRooted(request));
    }

    /**
     * Parses an action envelope into the request message it must satisfy.
     *
     * <p>The envelope is the message's canonical proto3 JSON form, so the same document works
     * over the catalog, over the JSON gateway, and as a tool call. Unknown members are refused
     * rather than ignored: a caller that misspells a field has written a request it did not
     * mean, and silently dropping it would do something else.
     */
    static <B extends Message.Builder> B parse(ObjectNode input, B builder, String verb)
            throws ActionException {
        try {
            JsonFormat.parser().merge(input.toString(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new ActionException("invalid-input",
                    verb + " expects a " + builder.getDescriptorForType().getName()
                            + ": " + e.getMessage());
        }
        ValidationResult result = VALIDATOR.validate(builder.build());
        if (!result.valid()) {
            throw new ActionException("invalid-input",
                    verb + " does not satisfy the request contract: " + describe(result),
                    violations(result));
        }
        return builder;
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

    /** Renders a protobuf message as a Jackson tree via canonical proto3 JSON. */
    static ObjectNode render(Message message, ActionContext context) throws ActionException {
        try {
            JsonNode node = context.objectMapper().readTree(
                    JsonFormat.printer().omittingInsignificantWhitespace().print(message));
            if (node instanceof ObjectNode object) {
                return object;
            }
            throw new IllegalStateException("protobuf JSON was not an object");
        } catch (JsonProcessingException e) {
            throw new ActionException("render-failed", "Failed to render protobuf JSON", null);
        } catch (Exception e) {
            throw new ActionException("render-failed", e.getMessage(), null);
        }
    }

    /** The named worker has no live bridge session. */
    static ActionException unknownWorker(String workerId) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("workerId", workerId);
        return new ActionException("unknown-worker",
                "Worker '" + workerId + "' is not registered; call delegation-worker-register",
                details);
    }

    /** The worker's delegation stream already failed; re-register to continue. */
    static ActionException streamFailed(String workerId, String detail) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("workerId", workerId);
        return new ActionException("worker-stream-failed",
                "Worker stream for '" + workerId + "' failed: " + detail, details);
    }

    /** The coordinator refused the operation (unknown task, stale phase, bad frame). */
    static ActionException rejected(String detail) {
        return new ActionException("delegation-rejected", detail);
    }
}
