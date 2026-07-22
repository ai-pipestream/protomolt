package ai.pipestream.proto.actions;

import ai.pipestream.proto.json.MalformedProtobufJsonException;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

/** Validates a JSON message against the validation rules declared on its protobuf schema. */
final class ValidateMessageAction implements ProtoAction {

    @Override
    public String name() {
        return "validate-message";
    }

    @Override
    public String description() {
        return "Validates a JSON message against the validation rules declared on its protobuf "
                + "schema (ai.pipestream.proto.validate.v1 options); returns valid:true/false plus "
                + "one violation per broken rule with the field path, rule id and message.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("schema", ActionJson.schemaSourceSchema());
        properties.set("type", ActionJson.typeProperty(
                "Fully qualified message type to validate against; required unless the schema "
                        + "already identifies a single message."));
        properties.putObject("message")
                .put("type", "object")
                .put("description", "The message to validate, as canonical proto3 JSON.");
        ActionJson.required(schema, "schema", "message");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(Inputs.optionalString(input, "type"), "/type");
        ObjectNode messageNode = Inputs.requireObject(input, "message");
        DynamicMessage message;
        try {
            message = context.transcoder().fromJsonDynamic(messageNode.toString(), descriptor);
        } catch (MalformedProtobufJsonException e) {
            ObjectNode details = JsonNodeFactory.instance.objectNode();
            details.put("pointer", "/message");
            details.put("type", descriptor.getFullName());
            details.put("detail", e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            throw new ActionException("invalid-message",
                    "Message is not valid proto3 JSON for " + descriptor.getFullName() + ": "
                            + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()),
                    details);
        }
        ValidationResult result = ProtoValidator.forMessageType(descriptor).validate(message);
        ObjectNode output = context.objectMapper().createObjectNode();
        output.put("valid", result.valid());
        ArrayNode violations = output.putArray("violations");
        for (ValidationResult.Violation violation : result.violations()) {
            ObjectNode node = violations.addObject();
            node.put("field", violation.path());
            node.put("rule", violation.rulePath());
            node.put("ruleId", violation.ruleId());
            node.put("message", violation.message());
        }
        return output;
    }
}
