package ai.pipestream.proto.actions;

import ai.pipestream.proto.json.MalformedProtobufJsonException;
import ai.pipestream.proto.meta.SensitivityMasker;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Masks a message's fields by their schema-declared sensitivity classes — the masking
 * policy travels inside the descriptor, so every surface enforces exactly what the schema
 * authors declared.
 */
final class MaskMessageAction implements ProtoAction {

    @Override
    public String name() {
        return "mask-message";
    }

    @Override
    public String description() {
        return "Masks fields whose declared sensitivity class "
                + "(ai.pipestream.proto.meta.v1.field.sensitivity) is in 'classes' — e.g. "
                + "pii, secret. Strategies: 'remove' clears; 'redact' turns strings into "
                + "***; 'encrypt' seals string/bytes values with AES-GCM (reversible only "
                + "with the same key) and clears other types; 'decrypt' reverses encrypt "
                + "and fails loudly on a wrong key. Recurses through nested and repeated "
                + "messages, and into google.protobuf.Any payloads whose type the schema "
                + "carries. Returns the masked message, which field paths were touched, and "
                + "'unresolvedPayloads': packed payloads whose type the schema does not "
                + "describe, which therefore could not be masked.";
    }

    @Override
    public ObjectNode inputSchema() {
        ObjectNode schema = ActionJson.baseInputSchema();
        ObjectNode properties = schema.putObject("properties");
        properties.set("schema", ActionJson.schemaSourceSchema());
        properties.set("type", ActionJson.typeProperty(
                "Fully qualified message type; required unless the schema identifies one."));
        properties.putObject("message")
                .put("type", "object")
                .put("description", "The message to mask, as canonical proto3 JSON.");
        ObjectNode classes = properties.putObject("classes");
        classes.put("type", "array");
        classes.put("description", "Sensitivity classes to mask, e.g. [\"pii\"].");
        classes.putObject("items").put("type", "string");
        properties.putObject("strategy")
                .put("type", "string")
                .put("description", "'remove' (default), 'redact', 'encrypt', or 'decrypt'.");
        properties.putObject("key")
                .put("type", "string")
                .put("description", "Base64 AES key (16/24/32 bytes), required for "
                        + "encrypt/decrypt. The caller's key — never stored, never in the "
                        + "schema.");
        ActionJson.required(schema, "schema", "message", "classes");
        schema.put("additionalProperties", false);
        return schema;
    }

    @Override
    public ObjectNode execute(ObjectNode input, ActionContext context) throws ActionException {
        SchemaResolver.ResolvedSchema schema = SchemaResolver.resolve(input, "schema", context);
        Descriptor descriptor = schema.message(Inputs.optionalString(input, "type"), "/type");
        ObjectNode messageNode = Inputs.requireObject(input, "message");
        ArrayNode classesNode = Inputs.optionalArray(input, "classes");
        if (classesNode == null || classesNode.isEmpty()) {
            throw Inputs.invalidInput("'classes' must be a non-empty array", "/classes");
        }
        Set<String> classes = new LinkedHashSet<>(
                Inputs.stringElements(classesNode, "/classes"));
        String strategyName = Inputs.optionalString(input, "strategy");
        SensitivityMasker.Strategy strategy;
        try {
            strategy = strategyName == null
                    ? SensitivityMasker.Strategy.REMOVE
                    : SensitivityMasker.Strategy.of(strategyName);
        } catch (IllegalArgumentException e) {
            throw Inputs.invalidInput("'strategy' must be remove, redact, encrypt, or "
                    + "decrypt; got '" + strategyName + "'", "/strategy");
        }
        byte[] key = null;
        String keyText = Inputs.optionalString(input, "key");
        if (keyText != null) {
            try {
                key = java.util.Base64.getDecoder().decode(keyText);
            } catch (IllegalArgumentException e) {
                throw Inputs.invalidInput("'key' must be base64", "/key");
            }
        }
        if ((strategy == SensitivityMasker.Strategy.ENCRYPT
                || strategy == SensitivityMasker.Strategy.DECRYPT)
                && (key == null || (key.length != 16 && key.length != 24 && key.length != 32))) {
            throw Inputs.invalidInput("'key' must be an AES key of 16, 24, or 32 bytes for "
                    + strategy.name().toLowerCase(java.util.Locale.ROOT), "/key");
        }
        DynamicMessage message;
        try {
            message = context.transcoder().fromJsonDynamic(messageNode.toString(), descriptor);
        } catch (MalformedProtobufJsonException e) {
            throw Inputs.invalidInput("Message is not valid proto3 JSON for "
                    + descriptor.getFullName(), "/message");
        }
        // Packed payloads resolve against everything this call can see: the schema first,
        // which carries types the root proto never imports, then the registry, which is how a
        // payload type arrives when the schema was given inline.
        SensitivityMasker.PayloadResolver payloads = typeName -> {
            Descriptor found = schema.findMessage(typeName);
            return found != null ? found : context.registry().findDescriptorByFullName(typeName);
        };
        SensitivityMasker.MaskResult result;
        try {
            result = SensitivityMasker.mask(message, classes, strategy, key, payloads);
        } catch (IllegalArgumentException e) {
            // The key is validated above, so what reaches here is about the payload: a value
            // that is not one of our envelopes, or one this key cannot open.
            throw Inputs.invalidInput(e.getMessage(), "/message");
        }
        ObjectNode output = context.objectMapper().createObjectNode();
        output.set("message", ActionJson.messageToJson(result.message(), context));
        ArrayNode masked = output.putArray("maskedFields");
        for (String path : result.maskedPaths()) {
            masked.add(path);
        }
        if (!result.unresolvedPaths().isEmpty()) {
            // Reported only when it happened: these payloads were not masked, and the caller
            // is the only one who can say whether that is acceptable.
            ArrayNode unresolved = output.putArray("unresolvedPayloads");
            for (String path : result.unresolvedPaths()) {
                unresolved.add(path);
            }
        }
        return output;
    }
}
