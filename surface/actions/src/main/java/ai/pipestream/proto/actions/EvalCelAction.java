package ai.pipestream.proto.actions;

import ai.pipestream.proto.cel.CelCompilationException;
import ai.pipestream.proto.cel.CelEnvironmentFactory;
import ai.pipestream.proto.cel.CelEvaluationException;
import ai.pipestream.proto.cel.CelEvaluator;
import ai.pipestream.proto.json.MalformedProtobufJsonException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

import java.util.Map;

/** Evaluates a CEL expression over a JSON message typed by its protobuf schema. */
final class EvalCelAction implements ProtoAction {

    @Override
    public String name() {
        return "eval-cel";
    }

    @Override
    public String description() {
        return "Evaluates a CEL expression against a JSON message typed by its protobuf schema; "
                + "the message is bound as the variable 'input' (e.g. \"input.name + '!'\"), and "
                + "the result is returned as a JSON value with a type label.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("schema", ActionJson.schemaSourceSchema());
        properties.set("type", ActionJson.typeProperty(
                "Fully qualified message type of the input message; required unless the schema "
                        + "already identifies a single message."));
        properties.putObject("message")
                .put("type", "object")
                .put("description", "The message to bind as 'input', as canonical proto3 JSON.");
        properties.putObject("expression")
                .put("type", "string")
                .put("description",
                        "CEL expression over the variable 'input', type-checked against the message type.");
        ActionJson.required(schema, "schema", "message", "expression");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(Inputs.optionalString(input, "type"), "/type");
        ObjectNode messageNode = Inputs.requireObject(input, "message");
        String expression = Inputs.requireString(input, "expression");
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
        ObjectNode output = context.objectMapper().createObjectNode();
        output.set("result", ActionJson.celValue(value, context));
        output.put("resultType", ActionJson.celType(value));
        return output;
    }
}
