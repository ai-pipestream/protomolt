package ai.pipestream.proto.actions;

import ai.pipestream.proto.cel.CelCompilationException;
import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.http.json.MalformedProtobufJsonException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import java.util.Map;

/** Evaluates a CEL expression over a JSON message typed by its protobuf schema. */
final class EvalCelAction implements ProtoAction {

    @Override
    public String name() {
        return "eval-cel";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Evaluates a CEL expression against a JSON message typed by its protobuf schema; "
                + "the message is bound as the variable 'input' (e.g. \"input.name + '!'\"), and "
                + "the result is returned as a JSON value with a type label.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("EvalCelRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("EvalCelResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(named(input, "type"), "/type");
        // The message is a structure: its shape is the named type, not this contract.
        ObjectNode messageNode = Fields.json(input, "message");
        String expression = Fields.string(input, "expression");
        DynamicMessage message;
        try {
            message = context.transcoder().fromJsonDynamic(messageNode.toString(), descriptor);
        } catch (MalformedProtobufJsonException e) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("pointer", "/message");
            details.put("detail", e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            throw new ActionException("invalid-message",
                    "Message is not valid proto3 JSON for " + descriptor.getFullName(), details);
        }
        CelEvaluator evaluator = new CelEvaluator(
                CelEnvironmentFactory.builder().addMessageVar("input", descriptor).build());
        Object value;
        try {
            value = evaluator.evaluateValue(expression, Map.of("input", message));
        } catch (CelCompilationException e) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("expression", expression);
            details.put("detail", e.getMessage());
            throw new ActionException("invalid-expression",
                    "CEL expression does not compile: " + e.getMessage(), details);
        } catch (CelEvaluationException e) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("expression", expression);
            details.put("detail", e.getMessage());
            throw new ActionException("evaluation-failed",
                    "CEL expression failed at runtime: " + e.getMessage(), details);
        }
        return Reply.of(responseType())
                .set("result", ActionJson.celValue(value, context))
                .set("resultType", ActionJson.celType(value))
                .build();
    }

    /** A named type, or null when the caller left the schema's own default to apply. */
    private static String named(Message input, String field) {
        String value = Fields.string(input, field);
        return value.isEmpty() ? null : value;
    }

}
