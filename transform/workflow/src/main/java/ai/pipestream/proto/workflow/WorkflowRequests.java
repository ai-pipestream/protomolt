package ai.pipestream.proto.workflow;

import ai.pipestream.proto.actions.ActionException;
import ai.pipestream.proto.actions.Fields;
import ai.pipestream.proto.grpc.workflow.WorkflowValidation;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;

/** Reading a workflow request, and the refusals the workflow verbs share. */
final class WorkflowRequests {

    private WorkflowRequests() {
    }

    /** A required string on a request, refusing a blank one by name. */
    static String text(Message input, String field) throws ActionException {
        String value = Fields.string(input, field);
        if (value.isBlank()) {
            throw invalid("'" + field + "' must be a non-empty string", "/" + field);
        }
        return value;
    }

    /** An optional string on a request; blank means the caller said nothing. */
    static String optionalText(Message input, String field) {
        String value = Fields.string(input, field);
        return value.isBlank() ? null : value;
    }

    /** A required workflow identity, held to the same name rules the store enforces. */
    static String identity(Message input, String field) throws ActionException {
        return checkedName(text(input, field), field);
    }

    /** An optional workflow identity. */
    static String optionalIdentity(Message input, String field) throws ActionException {
        String value = optionalText(input, field);
        return value == null ? null : checkedName(value, field);
    }

    private static String checkedName(String value, String field) throws ActionException {
        try {
            WorkflowValidation.validateName(value, field);
            return value;
        } catch (IllegalArgumentException e) {
            throw invalid(e.getMessage(), "/" + field);
        }
    }

    /**
     * A {@code map<string, string>} of base64 payloads, decoded. Null when the caller sent
     * none, which is distinct from sending an empty one only in that both mean the same.
     */
    static java.util.Map<String, byte[]> base64Map(Message input, String field)
            throws ActionException {
        java.util.Map<String, String> encoded = Fields.map(input, field);
        if (encoded.isEmpty()) {
            return null;
        }
        java.util.Map<String, byte[]> values = new java.util.HashMap<>();
        for (var entry : encoded.entrySet()) {
            try {
                values.put(entry.getKey(),
                        java.util.Base64.getDecoder().decode(entry.getValue()));
            } catch (IllegalArgumentException e) {
                throw invalid("'" + field + "' value is not valid base64",
                        "/" + field + "/" + entry.getKey());
            }
        }
        return values;
    }

    static ActionException invalid(String message, String pointer) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("pointer", pointer);
        return new ActionException("invalid-input", message, details);
    }

    static ActionException unavailable(String capability, String remedy) {
        ObjectNode details = JsonNodeFactory.instance.objectNode();
        details.put("capability", capability);
        details.put("remedy", remedy);
        return new ActionException("unavailable", capability + " is not configured; " + remedy,
                details);
    }
}
