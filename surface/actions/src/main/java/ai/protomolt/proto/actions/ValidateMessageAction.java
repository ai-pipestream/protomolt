package ai.protomolt.proto.actions;

import ai.protomolt.proto.http.json.MalformedProtobufJsonException;
import ai.protomolt.proto.validate.ProtoValidator;
import ai.protomolt.proto.validate.ValidationResult;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

/** Validates a JSON message against the validation rules declared on its protobuf schema. */
final class ValidateMessageAction implements ProtoAction {

    @Override
    public String name() {
        return "validate-message";
    }

    @Override
    public String requiredScope() {
        return Scopes.SCHEMA_READ;
    }

    @Override
    public String description() {
        return "Validates a JSON message against the validation rules declared on its protobuf "
                + "schema (ai.pipestream.proto.validate.v1 options); returns valid:true/false plus "
                + "one violation per broken rule with the field path, rule id and message.";
    }

    @Override
    public Descriptor requestType() {
        return CatalogContract.request("ValidateMessageRequest");
    }

    @Override
    public Descriptor responseType() {
        return CatalogContract.response("ValidateMessageResponse");
    }

    @Override
    public Message execute(Message input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(named(input, "type"), "/type");
        // The message is a structure: its shape is the named type, not this contract.
        ObjectNode messageNode = Fields.json(input, "message");
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
        Reply output = Reply.of(responseType()).set("valid", result.valid());
        for (ValidationResult.Violation violation : result.violations()) {
            output.append("violations")
                    .set("field", violation.path())
                    .set("rule", violation.rulePath())
                    .set("ruleId", violation.ruleId())
                    .set("message", violation.message())
                    .build();
        }
        return output.build();
    }

    /** A named type, or null when the caller left the schema's own default to apply. */
    private static String named(Message input, String field) {
        String value = Fields.string(input, field);
        return value.isEmpty() ? null : value;
    }

}
